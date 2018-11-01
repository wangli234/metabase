import React from "react";
import { Box, Flex } from "grid-styled";
import { Motion, spring } from "react-motion";
import cx from "classnames";
import { findDOMNode } from "react-dom";

import fitViewport from "metabase/hoc/FitViewPort";
import QuestionAndResultLoader from "metabase/containers/QuestionAndResultLoader";
import Visualization from "metabase/visualizations/components/Visualization";

import { Absolute } from "metabase/components/Position";
import Button from "metabase/components/Button";
import Card from "metabase/components/Card";
import Icon, { IconWrapper } from "metabase/components/Icon";

import colors from "metabase/lib/colors";

/* Panels */
import ReferencePanel from "metabase/query_builder/containers/ReferencePanel";
import VisualizationPanel, {
  VisualiztionControls,
} from "metabase/query_builder/containers/VisualizationPanel";
import ResultPanel from "metabase/query_builder/containers/ResultPanel";

const HeaderControls = ({
  onToggleReference,
  onToggleVisualization,
  onToggleFilters,
}) => (
  <Flex align="center">
    <Motion
      defaultStyle={{ scale: 0 }}
      style={{ scale: onToggleVisualization ? spring(1) : spring(0) }}
    >
      {({ scale }) => (
        <Button
          medium
          mx={2}
          primary
          onClick={() => onToggleVisualization()}
          style={{ transform: `scale(${scale})` }}
        >
          Visualize
        </Button>
      )}
    </Motion>
    <IconWrapper onClick={() => onToggleFilters()}>
      <Icon name="star" />
    </IconWrapper>
    <IconWrapper onClick={() => onToggleReference()}>
      <Icon name="reference" />
    </IconWrapper>
  </Flex>
);

class FilterBar extends React.Component {
  render() {
    const { question } = this.props;
    window.q = question;
    return (
      <Flex align="center" px={3} pb={1}>
        <Card p={1} color="white" bg={colors["accent7"]} mr={1}>
          Filter 1
        </Card>
        <Card p={1} color="white" bg={colors["accent7"]}>
          Filter 2
        </Card>
      </Flex>
    );
  }
}

@fitViewport
class ModeContainer extends React.Component {
  state = {
    vizPanelOpen: false,
    referencePanelOpen: false,
    showResultPane: false,
    showFilterBar: false,
  };
  render() {
    const {
      referencePanelOpen,
      vizPanelOpen,
      showResultPane,
      showFilterBar,
    } = this.state;
    const wrapperHeight =
      findDOMNode(this.container) && findDOMNode(this.container).offsetHeight;
    return (
      <Flex
        flexDirection="column"
        w={"100%"}
        className={cx(this.props.fitClassNames, "overflow-hidden")}
      >
        <QuestionAndResultLoader questionId={this.props.params.q1}>
          {({ question, result, cancel, reload, rawSeries, loading }) => {
            if (!question) {
              return <div>"Loading..."</div>;
            }
            return (
              <Box className="flex flex-column flex-full">
                <Flex align="center" p={2} px={3}>
                  <h2>{question.displayName()}</h2>
                  <Box ml="auto">
                    <HeaderControls
                      onToggleReference={() =>
                        this.setState({
                          referencePanelOpen: !this.state.referencePanelOpen,
                        })
                      }
                      onToggleVisualization={
                        this.state.showResultPane
                          ? () => this.setState({ showResultPane: false })
                          : null
                      }
                      onToggleFilters={() =>
                        this.setState({
                          showFilterBar: !this.state.showFilterBar,
                        })
                      }
                    />
                  </Box>
                </Flex>
                <Motion
                  defaultStyle={{
                    height: 0,
                  }}
                  style={{
                    height: showFilterBar ? spring(60) : spring(0),
                  }}
                >
                  {({ height }) => (
                    <Box style={{ height, overflow: "hidden" }} w={"100%"}>
                      <FilterBar question={question} />
                    </Box>
                  )}
                </Motion>
                <Box
                  className="flex-full relative"
                  ref={node => (this.container = node)}
                >
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: vizPanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: vizPanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute
                        top={20}
                        bottom={80}
                        style={{
                          opacity,
                          zIndex: 2,
                          transform: `translate3d(${x}px, 0px, 0px)`,
                        }}
                      >
                        <VisualizationPanel
                          question={question}
                          onClosePanel={() =>
                            this.setState({ vizPanelOpen: false })
                          }
                        />
                      </Absolute>
                    )}
                  </Motion>
                  <Motion
                    defaultStyle={{ x: 0, y: 0, opacity: 0 }}
                    style={{
                      x: referencePanelOpen ? spring(40) : spring(0),
                      y: spring(10),
                      opacity: referencePanelOpen ? spring(1) : spring(0),
                    }}
                  >
                    {({ x, y, opacity }) => (
                      <Absolute
                        right={0}
                        style={{
                          transform: `translate3d(-${x}px, ${y}px, 0px)`,
                          opacity,
                          zIndex: 4,
                        }}
                      >
                        <ReferencePanel
                          question={question}
                          open={referencePanelOpen}
                        />
                      </Absolute>
                    )}
                  </Motion>
                  <Motion
                    defaultStyle={{ left: 80, right: 0 }}
                    style={{
                      left: vizPanelOpen ? spring(400) : spring(60),
                      right: referencePanelOpen ? spring(360) : spring(0),
                    }}
                  >
                    {({ left, right }) => (
                      <Box style={{ height: "80vh" }} mr={right} ml={left}>
                        {rawSeries && <Visualization rawSeries={rawSeries} />}
                      </Box>
                    )}
                  </Motion>
                  <Absolute
                    top={0}
                    bottom={40}
                    style={{ display: "flex", zIndex: 3 }}
                  >
                    <Flex
                      align="center"
                      justify="center"
                      flexDirection="column"
                      px={1}
                    >
                      <VisualiztionControls
                        question={question}
                        isOpen={vizPanelOpen}
                        onOpenPanel={() =>
                          this.setState({
                            vizPanelOpen: !this.state.vizPanelOpen,
                          })
                        }
                      />
                    </Flex>
                  </Absolute>
                  <Motion
                    defaultStyle={{ y: wrapperHeight - 60 }}
                    style={{
                      y: showResultPane
                        ? spring(0)
                        : spring(wrapperHeight - 60),
                    }}
                  >
                    {({ y }) => (
                      <Absolute
                        bottom={0}
                        left={0}
                        right={0}
                        top={0}
                        onClick={() =>
                          this.setState({
                            showResultPane: !this.state.showResultPane,
                          })
                        }
                        style={{
                          zIndex: 3,
                          backgroundColor: "white",
                          transform: `translate3d(0px, ${y}px, 0px)`,
                        }}
                      >
                        <ResultPanel
                          dataId={this.props.params.q2}
                          aggregationId={this.props.params.q3}
                        />
                      </Absolute>
                    )}
                  </Motion>
                </Box>
              </Box>
            );
          }}
        </QuestionAndResultLoader>
      </Flex>
    );
  }
}

export default ModeContainer;
