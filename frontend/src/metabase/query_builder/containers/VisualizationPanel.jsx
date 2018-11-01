import React from "react";
import { Box, Flex } from "grid-styled";
import { Motion, spring } from "react-motion";

import visualizations from "metabase/visualizations";

import colors from "metabase/lib/colors";

import Card from "metabase/components/Card";
import Icon, { IconWrapper } from "metabase/components/Icon";
import { Grid, GridItem } from "metabase/components/Grid";

export class VisualiztionControls extends React.Component {
  render() {
    const { isOpen } = this.props;
    return (
      <Flex
        align="center"
        justify="center"
        flexDirection="column"
        style={{ height: "100%" }}
      >
        <Card
          style={{ borderRadius: 10000, lineHeight: 1 }}
          p={2}
          mb={1}
          className="text-brand cursor-pointer"
          onClick={() => this.props.onOpenPanel()}
        >
          <Icon name={isOpen ? "chevronleft" : "bar"} />
        </Card>
      </Flex>
    );
  }
}

const PanelHeader = ({ children }) => <h3>{children}</h3>;

class VisualizationPanel extends React.Component {
  render() {
    const { question, onClosePanel } = this.props;
    return (
      <Card p={2} style={{ width: 320, height: "100%" }}>
        <PanelHeader>What do you want to see?</PanelHeader>
        <Box mt={3}>
          <Grid>
            {Array.from(visualizations).map(([vizType, viz], index) => (
              <GridItem key={index} w={1 / 2} my={2}>
                <Flex
                  align="center"
                  flexDirection="column"
                  className="text-brand-hover cursor-pointer"
                  onClick={() => onClosePanel()}
                >
                  <Icon
                    name={viz.iconName}
                    size={18}
                    mb={1}
                    color={colors["bg-dark"]}
                  />
                  <h4>{viz.uiName}</h4>
                </Flex>
              </GridItem>
            ))}
          </Grid>
        </Box>
      </Card>
    );
  }
}

export default VisualizationPanel;
